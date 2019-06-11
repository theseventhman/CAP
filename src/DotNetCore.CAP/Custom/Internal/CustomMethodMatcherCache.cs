using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DotNetCore.CAP.Custom.Internal
{
    internal class CustomMethodMatcherCache
    {
        private readonly IConsumerServiceSelector _selector;

        private ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>> Entries { get; }

        public CustomMethodMatcherCache(IConsumerServiceSelector selector)
        {
            _selector = selector;
            Entries = new ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>>();
        }

        /// <summary>
        /// Get a dictionary of candidates.In the dictionary.
        /// the Key is the CAPSubscribeAttribute Group, the Value for the current Group of candidates
        /// </summary>
        /// <returns></returns>
        public ConcurrentDictionary<string,IReadOnlyList<ConsumerExecutorDescriptor>> GetCandidatesMethodsOfGroupNameGrouped()
        {
            if(Entries.Count !=0)
            {
                return Entries;
            }

            var executorCollection = _selector.SelectCandidates();

            var groupedCandidates = executorCollection.GroupBy(x => x.Attribute.Group);

            foreach(var item in groupedCandidates)
            {
                Entries.TryAdd(item.Key, item.ToList());
            }

            return Entries;
        }

        /// <summary>
        /// Attempts to get the topic executor associated with the specified topic name and group name from the 
        /// <see cref="Entries"/>
        /// </summary>
        /// <param name="topicName">The topic name of the value to get.</param>
        /// <param name="groupName">The group name of the value to get.</param>
        /// <param name="matchTopic">topic executor of the value.</param>
        /// <returns>true if the key was found, otherwise false.</returns>
       
        public bool TryGetTopicExecutor(string topicName, string groupName, out ConsumerExecutorDescriptor matchTopic)
        {
            if(Entries == null)
            {
                throw new ArgumentNullException(nameof(Entries));
            }

            matchTopic = null;
            IReadOnlyList<ConsumerExecutorDescriptor> list;

            if( Entries.TryGetValue(groupName, out var groupMatchTopics))
            {
                matchTopic = _selector.SelectBestCandidate(topicName, groupMatchTopics);
                return matchTopic != null;
            }
            return false;

        }
    }
}
